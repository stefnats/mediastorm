package api

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"novastream/models"
)

// These requests use actual Register/account/PIN middleware and controlled
// existing recording storage. They do not establish a real joined media file.
func TestRecordingDerivedRegisteredRoutesRequireOriginalAccountOwnerAndPIN(t *testing.T) {
	f := newTimingAccessFixture(t)
	for _, operation := range []struct{ method, suffix string }{
		{"POST", "/derived/join"}, {"GET", "/derived"},
		{"GET", "/derived/0123456789abcdef0123456789abcdef/stream"},
		{"HEAD", "/derived/0123456789abcdef0123456789abcdef/stream"},
	} {
		t.Run(operation.method+operation.suffix, func(t *testing.T) {
			target := func(profile string) string {
				return "/api/live/recordings/job-" + url.PathEscape(profile) + operation.suffix
			}
			cases := []struct {
				name    string
				status  int
				request func() pinRequest
			}{
				{"unauthenticated", 401, func() pinRequest { return pinRequest{method: operation.method, target: target(f.open)} }},
				{"invalid session", 401, func() pinRequest {
					return pinRequest{method: operation.method, target: target(f.open), token: "synthetic-invalid"}
				}},
				{"foreign owner", 404, func() pinRequest {
					return pinRequest{method: operation.method, target: target(f.foreign), token: f.login(f.account1)}
				}},
				{"locked owner", 403, func() pinRequest {
					return pinRequest{method: operation.method, target: target(f.locked), token: f.login(f.account1)}
				}},
				{"child owner", 403, func() pinRequest {
					return pinRequest{method: operation.method, target: target(f.child), token: f.login(f.account1)}
				}},
				{"scoped session", 403, func() pinRequest {
					session, err := f.sessions.CreateScoped(f.account1, false, "synthetic-derived", "loopback", time.Hour, models.SessionScopeStream)
					if err != nil {
						t.Fatal(err)
					}
					return pinRequest{method: operation.method, target: target(f.open), token: session.Token}
				}},
				{"foreign session unlock", 403, func() pinRequest {
					first := f.login(f.account1)
					unlock := f.verify(first, f.locked, "1234").UnlockToken
					return pinRequest{method: operation.method, target: target(f.locked), token: f.login(f.account1), unlock: unlock}
				}},
				{"conflicting selector", 404, func() pinRequest {
					token := f.login(f.account1)
					return pinRequest{method: operation.method, target: target(f.open) + "?profileId=" + url.QueryEscape(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
				}},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					f.pinTestEnv.t = t
					w := f.do(tc.request())
					if w.Code != tc.status || strings.Contains(w.Body.String(), "PRIVATE-") || w.Header().Get("Content-Type") == "video/x-matroska" {
						t.Fatalf("unsafe registered derived response: %d", w.Code)
					}
				})
			}
		})
	}
}
