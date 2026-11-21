//go:build linux

package jellyfinbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/jellyfinbridge/dto"
	"novastream/internal/settingscommit"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
)

const publicQCApproverToken = "synthetic-public-qc-approver"
const publicQCTargetClient = "target-public-device"
const publicQCApproverClient = "approver-public-device"

type publicQCFixture struct {
	*bridgeLogoutFixture
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	runtime     *ManagedPasswordLogin
	wire        *httptest.Server
	client      *http.Client
	actorToken  string
	endpoint    string
	proxy       *publicQCProxy
}

func publicQCPG(t *testing.T, oneSlot bool, dropAt int32, lifetime ...context.Context) *publicQCFixture {
	t.Helper()
	base, _ := bridgeLogoutPG(t, false)
	f := &publicQCFixture{bridgeLogoutFixture: base, actorToken: publicQCApproverToken}
	if oneSlot || dropAt != 0 {
		u, err := url.Parse(f.ds.Pool().Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		if dropAt != 0 {
			f.proxy = publicQCProxyStart(t, u)
			query := u.Query()
			query.Set("host", f.proxy.directory)
			query.Set("sslmode", "disable")
			u.RawQuery = query.Encode()
		}
		f.ds.Close()
		maximum := 4
		if oneSlot {
			maximum = 1
		}
		f.ds, err = datastore.New(t.Context(), u.String(), datastore.WithMaxConns(maximum), datastore.WithMinConns(0))
		if err != nil {
			t.Fatal(err)
		}
		f.checked.Service, err = sessions.NewServiceWithStore(f.ds, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(managedPasswordTestSecret), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.ds.Pool().Exec(t.Context(), `DELETE FROM sessions;INSERT INTO user_settings_versions(user_id,generation)VALUES('target',0) ON CONFLICT(user_id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.ds.Pool().Exec(t.Context(), `UPDATE accounts SET password_hash=$1 WHERE id='actor'`, string(hash)); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	approver := models.Session{Token: publicQCApproverToken, AccountID: "actor", ClientID: publicQCApproverClient, Scope: models.SessionScopeJellyfin, ScopeResource: "target", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err = f.ds.Sessions().Create(t.Context(), &approver); err != nil {
		t.Fatal(err)
	}
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = f.ds.Pool().Config().ConnString()
	settings.JellyfinBridge.Enabled = true
	settings.JellyfinBridge.QuickConnect = true
	settings.Security.EnforceProfilePins = false
	if err = f.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	objects, err := settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.security != nil {
			_ = f.security.Close()
		}
		if f.coordinator != nil {
			_ = f.coordinator.Close()
		}
		_ = objects.Close()
	})
	f.coordinator, err = settingscommit.NewForPool(t.Context(), f.ds.Pool(), objects)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.coordinator.Arm(guard); err != nil {
		_ = guard.Close()
		t.Fatal(err)
	}
	f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	_ = guard.Close()
	if err != nil {
		t.Fatal(err)
	}
	life := t.Context()
	if len(lifetime) > 0 {
		life = lifetime[0]
	}
	f.runtime, err = NewManagedPasswordLogin(life, f.ds, f.manager, f.security, f.coordinator, profileunlock.NewPGProfilePins(f.ds))
	if err != nil {
		t.Fatal(err)
	}
	old := f.server
	deps := old.deps
	deps.ManagedPassword = f.runtime
	_ = old.Shutdown(context.Background())
	f.server = New(deps, old.cfg)
	if f.server.managedQC == nil {
		t.Fatal("actual managed per-Server QC composition unavailable")
	}
	f.wire = httptest.NewServer(f.server.Handler())
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	f.client = &http.Client{Timeout: 8 * time.Second, Transport: transport}
	t.Cleanup(func() {
		_ = f.server.Shutdown(context.Background())
		if f.wire != nil {
			f.wire.Close()
		}
		transport.CloseIdleConnections()
	})
	return f
}
func (f *publicQCFixture) request(t *testing.T, ctx context.Context, method, path, client, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	endpoint := f.wire.URL
	if f.endpoint != "" {
		endpoint = f.endpoint
	}
	r, err := http.NewRequestWithContext(ctx, method, endpoint+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", fmt.Sprintf(`MediaBrowser Client="Synthetic%%20TV", Version="3.1", Device="Target%%20Screen", DeviceId="%s", Token="%s"`, client, token))
	response, err := f.client.Do(r)
	if err != nil {
		t.Fatal("actual QC HTTP transport", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 128*1024))
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	w.Code = response.StatusCode
	w.HeaderMap = response.Header.Clone()
	_, _ = w.Body.Write(data)
	return w
}
func (f *publicQCFixture) initiate(t *testing.T) dto.QuickConnectResult {
	t.Helper()
	w := f.request(t, t.Context(), http.MethodPost, "/QuickConnect/Initiate", publicQCTargetClient, "", nil)
	var result dto.QuickConnectResult
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.Secret) != 64 || result.Authenticated {
		t.Fatal("actual QC initiation", w.Code)
	}
	return result
}
func (f *publicQCFixture) authorize(t *testing.T, result dto.QuickConnectResult) *httptest.ResponseRecorder {
	t.Helper()
	return f.request(t, t.Context(), http.MethodPost, "/QuickConnect/Authorize?Code="+result.Code, publicQCApproverClient, f.actorToken, nil)
}
func (f *publicQCFixture) collect(t *testing.T, result dto.QuickConnectResult) *httptest.ResponseRecorder {
	t.Helper()
	return f.request(t, t.Context(), http.MethodPost, "/Users/AuthenticateWithQuickConnect", publicQCTargetClient, "", map[string]string{"Secret": result.Secret})
}
func (f *publicQCFixture) count(t *testing.T) int {
	t.Helper()
	var count int
	if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM `+pgx.Identifier{f.schema, "sessions"}.Sanitize()+` WHERE client_id=$1`, publicQCTargetClient).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func assertPublicQCError(t *testing.T, w *httptest.ResponseRecorder, want int) {
	t.Helper()
	if w.Code != want || strings.Contains(w.Body.String(), "AccessToken") || strings.Contains(w.Body.String(), publicQCApproverToken) {
		t.Fatal("generic QC failure or no-token contract", w.Code, want)
	}
}

func TestManagedQuickConnectWireFlow(t *testing.T) {
	for _, kind := range []string{"ordinary", "pool-one", "query-casing", "original-target-metadata"} {
		t.Run(kind, func(t *testing.T) {
			f := publicQCPG(t, kind == "pool-one", 0)
			w := f.request(t, t.Context(), http.MethodGet, "/QuickConnect/Enabled", "", "", nil)
			if w.Code != 200 || strings.TrimSpace(w.Body.String()) != "true" {
				t.Fatal("actual flag did not override conflicting cached default", w.Code)
			}
			if kind == "ordinary" {
				login := f.request(t, t.Context(), http.MethodPost, "/Users/AuthenticateByName", publicQCApproverClient, "", authenticateByNameRequest{Username: "Synthetic profile", Pw: managedPasswordTestSecret})
				var auth dto.AuthenticationResult
				if login.Code != 200 || json.Unmarshal(login.Body.Bytes(), &auth) != nil || auth.AccessToken == "" {
					t.Fatal("actual preceding password route", login.Code)
				}
				f.actorToken = auth.AccessToken
			}
			result := f.initiate(t)
			if result.DeviceID != publicQCTargetClient || result.DeviceName != "Target Screen" || result.AppName != "Synthetic TV" || result.AppVersion != "3.1" || result.DateAdded.IsZero() {
				t.Fatal("original initiation wire DATA")
			}
			secretKey := "secret"
			if kind == "query-casing" {
				secretKey = "SeCrEt"
			}
			poll := f.request(t, t.Context(), http.MethodGet, "/QuickConnect/Connect?"+secretKey+"="+result.Secret, "", "", nil)
			var before dto.QuickConnectResult
			if poll.Code != 200 || json.Unmarshal(poll.Body.Bytes(), &before) != nil || before.Authenticated {
				t.Fatal("unapproved polling wire")
			}
			if approved := f.authorize(t, result); approved.Code != 200 || strings.TrimSpace(approved.Body.String()) != "true" {
				t.Fatal("actual genuine PG approval", approved.Code)
			}
			poll = f.request(t, t.Context(), http.MethodGet, "/QuickConnect/Connect?"+secretKey+"="+result.Secret, "", "", nil)
			var after dto.QuickConnectResult
			if poll.Code != 200 || json.Unmarshal(poll.Body.Bytes(), &after) != nil || !after.Authenticated || after.DeviceID != result.DeviceID || after.DateAdded != result.DateAdded {
				t.Fatal("approved original polling wire")
			}
			collected := f.collect(t, result)
			if kind == "original-target-metadata" {
				// This is a second genuine flow: the collector changes descriptive
				// claims while retaining the original device/secret binding.
				second := f.initiate(t)
				if f.authorize(t, second).Code != 200 {
					t.Fatal("second genuine approval")
				}
				body, _ := json.Marshal(map[string]string{"Secret": second.Secret})
				request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, f.wire.URL+"/Users/AuthenticateWithQuickConnect", bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Authorization", `MediaBrowser Client="CollectorOther",Version="99",Device="DifferentScreen",DeviceId="target-public-device"`)
				response, err := f.client.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				collected = httptest.NewRecorder()
				collected.Code = response.StatusCode
				_, _ = collected.Body.Write(data)
			}
			var auth dto.AuthenticationResult
			if collected.Code != 200 || json.Unmarshal(collected.Body.Bytes(), &auth) != nil || auth.AccessToken == "" || auth.User.ID != deriveID(refUser, "target") || auth.User.Name != "Synthetic actor/Synthetic profile" {
				t.Fatal("known actual scoped authentication result", collected.Code)
			}
			var client, agent, scope, resource, peer string
			if err := f.ds.Pool().QueryRow(t.Context(), `SELECT client_id,user_agent,scope,scope_resource,ip_address FROM sessions WHERE token=$1`, auth.AccessToken).Scan(&client, &agent, &scope, &resource, &peer); err != nil {
				t.Fatal(err)
			}
			wantCount := 1
			if kind == "original-target-metadata" {
				wantCount = 2
			}
			if client != publicQCTargetClient || agent != "Jellyfin: Synthetic TV 3.1 Target Screen" || scope != models.SessionScopeJellyfin || resource != "target" || f.count(t) != wantCount || f.ds.Pool().Stat().AcquiredConns() != 0 {
				t.Fatal("actual original target metadata/scope or pool cleanup")
			}
			if host, _, err := net.SplitHostPort(peer); err != nil || host != "127.0.0.1" {
				t.Fatal("actual original collector peer was substituted")
			}
			assertPublicQCError(t, f.collect(t, result), http.StatusNotFound)
		})
	}
}

func TestManagedQuickConnectAuthorizerFrame(t *testing.T) {
	for _, kind := range []string{"other-user", "missing-token", "wrong-client", "full-scope", "stale-session", "profile-epoch", "unsupported-pin", "child", "household"} {
		t.Run(kind, func(t *testing.T) {
			f := publicQCPG(t, false, 0)
			result := f.initiate(t)
			path := "/QuickConnect/Authorize?code=" + result.Code
			token, client := f.actorToken, publicQCApproverClient
			switch kind {
			case "other-user":
				path += "&userId=" + deriveID(refUser, "different-profile")
			case "missing-token":
				token = ""
			case "wrong-client":
				client = "different-device"
			case "full-scope":
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE sessions SET scope='',scope_resource='' WHERE token=$1`, token); err != nil {
					t.Fatal(err)
				}
			case "stale-session":
				if _, err := f.checked.ValidateScoped(token, models.SessionScopeJellyfin); err != nil {
					t.Fatal(err)
				}
				if _, err := f.ds.Pool().Exec(t.Context(), `DELETE FROM sessions WHERE token=$1`, token); err != nil {
					t.Fatal(err)
				}
			case "profile-epoch":
				if f.authorize(t, result).Code != 200 {
					t.Fatal("initial genuine approval")
				}
				if _, err := f.ds.Pool().Exec(t.Context(), `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('profile','target',1)`); err != nil {
					t.Fatal(err)
				}
				assertPublicQCError(t, f.collect(t, result), 401)
				if f.count(t) != 0 {
					t.Fatal("revoked original profile approval minted")
				}
				return
			case "unsupported-pin":
				hash, err := bcrypt.GenerateFromPassword([]byte("2580"), bcrypt.MinCost)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.ds.Pool().Exec(t.Context(), `UPDATE users SET pin_hash=$1,pin_length=4 WHERE id='target'`, string(hash)); err != nil {
					t.Fatal(err)
				}
				settings, err := f.manager.Load()
				if err != nil {
					t.Fatal(err)
				}
				settings.Security.EnforceProfilePins = true
				if err = f.manager.Save(settings); err != nil {
					t.Fatal(err)
				}
			case "child":
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id='target'`); err != nil {
					t.Fatal(err)
				}
			case "household":
				if _, err := f.ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('child','actor','Synthetic child',true)`); err != nil {
					t.Fatal(err)
				}
			}
			want := 401
			if kind == "other-user" {
				want = 403
			}
			assertPublicQCError(t, f.request(t, t.Context(), http.MethodPost, path, client, token, nil), want)
			if f.count(t) != 0 {
				t.Fatal("unsupported/foreign approver created a session")
			}
			poll := f.request(t, t.Context(), http.MethodGet, "/QuickConnect/Connect?secret="+result.Secret, "", "", nil)
			var status dto.QuickConnectResult
			if poll.Code != 200 || json.Unmarshal(poll.Body.Bytes(), &status) != nil || status.Authenticated {
				t.Fatal("failed authorizer attached an approval", poll.Code)
			}
		})
	}
}

type publicQCLegacyTrap struct {
	*bridgeLogoutChecked
	called atomic.Int64
}

func (s *publicQCLegacyTrap) CreateScopedWithResource(string, bool, string, string, time.Duration, string, string) (models.Session, error) {
	s.called.Add(1)
	return models.Session{}, errors.New("legacy session creation must not run")
}

type publicQCUnknownStorage struct{ SessionService }

func TestManagedQuickConnectTerminalFailures(t *testing.T) {
	for _, kind := range []string{"missing-runtime", "unknown-storage", "closed-runtime", "disabled-policy", "policy-unavailable", "native-busy", "unknown-secret", "unapproved-secret", "reused-secret", "collector-client"} {
		t.Run(kind, func(t *testing.T) {
			f := publicQCPG(t, false, 0)
			result := f.initiate(t)
			want := 503
			if kind == "missing-runtime" || kind == "unknown-storage" {
				trap := &publicQCLegacyTrap{bridgeLogoutChecked: f.checked}
				deps := f.server.deps
				deps.ManagedPassword = nil
				deps.Sessions = trap
				if kind == "unknown-storage" {
					deps.Sessions = publicQCUnknownStorage{SessionService: trap}
				}
				f.wire.Close()
				old := f.server
				f.server = New(deps, old.cfg)
				_ = old.Shutdown(context.Background())
				f.wire = httptest.NewServer(f.server.Handler())
				assertPublicQCError(t, f.collect(t, result), 503)
				if trap.called.Load() != 0 {
					t.Fatal("database configuration fell through legacy mint")
				}
				return
			}
			switch kind {
			case "closed-runtime":
				f.server.managedQC.close()
			case "disabled-policy":
				settings, err := f.manager.Load()
				if err != nil {
					t.Fatal(err)
				}
				settings.JellyfinBridge.QuickConnect = false
				if err = f.manager.Save(settings); err != nil {
					t.Fatal(err)
				}
				enabled := f.request(t, t.Context(), http.MethodGet, "/QuickConnect/Enabled", "", "", nil)
				if enabled.Code != 200 || strings.TrimSpace(enabled.Body.String()) != "false" {
					t.Fatal("disabled current flag hidden")
				}
				want = 401
			case "policy-unavailable":
				path := f.manager.ConfigPath()
				if err := os.Rename(path, path+".synthetic-unavailable"); err != nil {
					t.Fatal(err)
				}
				defer os.Rename(path+".synthetic-unavailable", path)
			case "native-busy":
				guard, err := f.coordinator.TryWrite(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer guard.Close()
			case "unknown-secret":
				result.Secret = strings.Repeat("0", 64)
				want = 404
			case "unapproved-secret":
				want = 404
			case "reused-secret":
				if f.authorize(t, result).Code != 200 || f.collect(t, result).Code != 200 {
					t.Fatal("original successful collection")
				}
				want = 404
			case "collector-client":
				if f.authorize(t, result).Code != 200 {
					t.Fatal("actual approval")
				}
				assertPublicQCError(t, f.request(t, t.Context(), http.MethodPost, "/Users/AuthenticateWithQuickConnect", "wrong-target", "", map[string]string{"Secret": result.Secret}), 401)
				if f.count(t) != 0 {
					t.Fatal("substituted target client minted")
				}
				return
			}
			assertPublicQCError(t, f.collect(t, result), want)
			expected := 0
			if kind == "reused-secret" {
				expected = 1
			}
			if f.count(t) != expected {
				t.Fatal("failed collection changed actual target session count")
			}
		})
	}
}

func TestManagedQuickConnectWrongCodeBudget(t *testing.T) {
	f := publicQCPG(t, false, 0)
	for n := 0; n < 10; n++ {
		assertPublicQCError(t, f.request(t, t.Context(), http.MethodPost, "/QuickConnect/Authorize?CoDe=000000", publicQCApproverClient, f.actorToken, nil), 404)
	}
	assertPublicQCError(t, f.request(t, t.Context(), http.MethodPost, "/QuickConnect/Authorize?code=999999", publicQCApproverClient, f.actorToken, nil), 429)
}
func TestManagedQuickConnectConcurrentOwnership(t *testing.T) {
	for _, kind := range []string{"authorize", "collect"} {
		t.Run(kind, func(t *testing.T) {
			f := publicQCPG(t, false, 0)
			result := f.initiate(t)
			if kind == "collect" && f.authorize(t, result).Code != 200 {
				t.Fatal("actual approval")
			}
			var ready sync.WaitGroup
			ready.Add(2)
			start := make(chan struct{})
			results := make(chan int, 2)
			for n := 0; n < 2; n++ {
				go func() {
					code := 0
					defer func() { results <- code }()
					ready.Done()
					<-start
					if kind == "authorize" {
						code = f.authorize(t, result).Code
					} else {
						code = f.collect(t, result).Code
					}
				}()
			}
			ready.Wait()
			close(start)
			first, second := <-results, <-results
			if first != 200 && second != 200 || first == 200 && second == 200 {
				t.Fatal("concurrent operation did not have exactly one owner", first, second)
			}
			if kind == "authorize" {
				if first != 400 && second != 400 {
					t.Fatal("duplicate approval wire", first, second)
				}
				if f.collect(t, result).Code != 200 {
					t.Fatal("actual winning approval unusable")
				}
			} else if first != 404 && second != 404 {
				t.Fatal("repeated collection wire", first, second)
			}
			if f.count(t) != 1 {
				t.Fatal("concurrent ownership minted more than once")
			}
		})
	}
}
func TestManagedQuickConnectUnknownACK(t *testing.T) {
	for _, kind := range []string{"mint", "release"} {
		t.Run(kind, func(t *testing.T) {
			dropAt := int32(1)
			if kind == "release" {
				dropAt = 2
			}
			f := publicQCPG(t, false, dropAt)
			result := f.initiate(t)
			if f.authorize(t, result).Code != 200 {
				t.Fatal("actual approval before arming actual ACK loss")
			}
			f.proxy.dropAfter.Store(dropAt)
			assertPublicQCError(t, f.collect(t, result), 503)
			if !f.proxy.dropped.Load() || f.count(t) != 1 {
				t.Fatal("actual stored mint/dropped COMMIT ACK witness absent", kind)
			}
			assertPublicQCError(t, f.collect(t, result), 404)
		})
	}
}

func TestManagedQuickConnectPollPublicationBoundary(t *testing.T) {
	f := publicQCPG(t, false, 0)
	result := f.initiate(t)
	m := f.server.managedQC
	entry := m.lookup(result.Secret, "")
	if entry == nil || !m.claimApproval(entry) {
		t.Fatal("actual wrapper approval claim")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	input := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://synthetic.invalid/QuickConnect/Authorize", nil)
	input.RemoteAddr = "198.51.100.8:1234"
	input.Header.Set("X-Client-ID", publicQCApproverClient)
	checked, release, err := f.runtime.pins.Authenticate(input, f.actorToken, publicQCApproverClient)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	approval, err := m.issuer.ApproveAuthenticated(checked, entry.challenge)
	if err != nil {
		t.Fatal(err)
	}
	// The genuine core handoff exists, while its real HTTP wrapper owner has
	// deliberately not attached/published that original approval yet.
	actual, err := entry.challenge.Details()
	if err != nil || !actual.Approved {
		t.Fatal("genuine core approval was not staged", err)
	}
	poll := f.request(t, t.Context(), http.MethodGet, "/QuickConnect/Connect?secret="+result.Secret, "", "", nil)
	var pending dto.QuickConnectResult
	if poll.Code != 200 || json.Unmarshal(poll.Body.Bytes(), &pending) != nil || pending.Authenticated {
		t.Fatal("poll advertised approval before original owner attached it", poll.Code)
	}
	assertPublicQCError(t, f.collect(t, result), 404)
	if f.count(t) != 0 {
		t.Fatal("unpublished original approval reached mint")
	}
	release()
	if !m.attachApproval(entry, approval) {
		t.Fatal("actual original approval attachment")
	}
	poll = f.request(t, t.Context(), http.MethodGet, "/QuickConnect/Connect?secret="+result.Secret, "", "", nil)
	var ready dto.QuickConnectResult
	if poll.Code != 200 || json.Unmarshal(poll.Body.Bytes(), &ready) != nil || !ready.Authenticated {
		t.Fatal("attached original approval was not advertised", poll.Code)
	}
	if collected := f.collect(t, result); collected.Code != 200 || f.count(t) != 1 {
		t.Fatal("actual attached handoff did not mint exactly once", collected.Code)
	}
	assertPublicQCError(t, f.collect(t, result), 404)
}
