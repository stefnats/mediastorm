//go:build linux

package settingsbootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"novastream/config"
	"novastream/internal/datastore"
)

type deploymentWebDAVStartupFixture struct {
	boot    *bootstrapPGFixture
	startup *Startup
	store   *datastore.DataStore
	issuer  *DeploymentWebDAVIssuer
}

func deploymentWebDAVStartupPG(t *testing.T) *deploymentWebDAVStartupFixture {
	t.Helper()
	f := &deploymentWebDAVStartupFixture{boot: bootstrapPG(t)}
	settings := config.DefaultSettings()
	settings.Database.URL = f.boot.raw
	settings.Database.MaxOpenConns = 1
	settings.Security.EnforceProfilePins = false
	settings.WebDAV = config.WebDAVSettings{Enabled: true, Prefix: "/webdav", Username: "synthetic-basic", Password: "synthetic-password"}
	if err := f.boot.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	var err error
	f.startup, err = Begin(t.Context(), f.boot.manager, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.startup.Close(); err != nil {
			t.Error(err)
		}
	})
	// No factory may silently start the original Finish.
	if issuer, e := f.startup.NewDeploymentWebDAVIssuer(nil); e == nil || issuer != nil {
		t.Fatal("unready Startup issued shared deployment observer")
	}
	f.store, err = f.startup.Migrate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.startup.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('original-account','Synthetic original','synthetic-unused-hash');INSERT INTO users(id,account_id,name)VALUES('original-profile','original-account','Synthetic profile')`); err != nil {
		t.Fatal(err)
	}
	f.issuer, err = f.startup.NewDeploymentWebDAVIssuer(f.store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.issuer.Close)
	return f
}
func deploymentWebDAVOriginalRequest(t *testing.T, ttl time.Duration, peer string) *http.Request {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), ttl)
	t.Cleanup(cancel)
	r := httptest.NewRequest(http.MethodGet, "http://synthetic.test/webdav/movie", nil).WithContext(ctx)
	r.RemoteAddr = peer
	r.SetBasicAuth("synthetic-basic", "synthetic-password")
	return r
}
func deploymentWebDAVOriginalProof(t *testing.T, f *deploymentWebDAVStartupFixture, r *http.Request) *DeploymentWebDAVApplicability {
	t.Helper()
	proof, err := f.issuer.Capture(r)
	if err != nil || proof == nil {
		t.Fatal("actual Startup/native Basic/deployment observation", err)
	}
	t.Cleanup(proof.Close)
	return proof
}
func deploymentWebDAVNativeReadHeld(t *testing.T, configPath string) bool {
	t.Helper()
	info, err := os.Stat(filepath.Join(filepath.Dir(configPath), ".mediastorm-settings-commit", "commit-barrier.lock"))
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("actual native lock identity missing")
	}
	identity := fmt.Sprintf("%02x:%02x:%d", unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev)), stat.Ino)
	raw, err := os.ReadFile("/proc/locks")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 8 && fields[1] == "FLOCK" && fields[3] == "READ" && fields[4] == strconv.Itoa(os.Getpid()) && fields[5] == identity {
			return true
		}
	}
	return false
}
func TestDeploymentWebDAVOriginal(t *testing.T) {
	for _, name := range []string{"genuine-original-basic", "zero-copy-foreign-issuer", "different-finalized-request", "fixed-expiry-no-renew", "close-joins-owned-query"} {
		t.Run(name, func(t *testing.T) {
			f := deploymentWebDAVStartupPG(t)
			ttl := 30 * time.Second
			if name == "fixed-expiry-no-renew" {
				ttl = 1500 * time.Millisecond
			}
			r := deploymentWebDAVOriginalRequest(t, ttl, "127.0.0.1:21001")
			if name == "genuine-original-basic" {
				tx, err := f.boot.observer.Begin(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(t.Context(), "LOCK TABLE users IN ACCESS EXCLUSIVE MODE"); err != nil {
					t.Fatal(err)
				}
				r.SetBasicAuth("synthetic-basic", "wrong-synthetic-password")
				proof, e := f.issuer.Capture(r)
				_ = tx.Rollback(context.Background())
				if !errors.Is(e, config.ErrWebDAVBasicRejected) || proof != nil {
					if proof != nil {
						proof.Close()
					}
					t.Fatalf("wrong Basic reached blocked PG or became permission: %v", e)
				}
				r.SetBasicAuth("synthetic-basic", "synthetic-password")
			}
			proof := deploymentWebDAVOriginalProof(t, f, r)
			switch name {
			case "genuine-original-basic":
				if err := proof.Check(r); err != nil {
					t.Fatal(err)
				}
				if proof.Prefix() != "/webdav" {
					t.Fatal("facade lost original native route DATA")
				}
				var sessions int
				if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions").Scan(&sessions); err != nil || sessions != 0 {
					t.Fatal("no-actor observer minted a session", err)
				}
			case "zero-copy-foreign-issuer":
				copied := *f.issuer
				if p, err := copied.Capture(r); err == nil || p != nil {
					if p != nil {
						p.Close()
					}
					t.Fatal("copied issuer accepted")
				}
				copyProof := *proof
				if copyProof.Check(r) == nil || copyProof.Prefix() != "" {
					t.Fatal("copied receipt accepted")
				}
				var zero DeploymentWebDAVApplicability
				if zero.Check(r) == nil || json.Unmarshal([]byte("{}"), &zero) == nil || zero.Context().Err() == nil || !zero.ExpiresAt().IsZero() || zero.Prefix() != "" || (*DeploymentWebDAVApplicability)(nil).Prefix() != "" {
					t.Fatal("zero/imported applicability accepted")
				}
				foreign, err := datastore.New(t.Context(), f.boot.raw)
				if err != nil {
					t.Fatal(err)
				}
				if other, e := f.startup.NewDeploymentWebDAVIssuer(foreign); e == nil || other != nil {
					if other != nil {
						other.Close()
					}
					foreign.Close()
					t.Fatal("foreign actual store borrowed original Startup")
				}
				foreign.Close()
				raw, err := json.Marshal(proof)
				if err != nil || string(raw) != "null" || strings.Contains(fmt.Sprintf("%+v", proof), "synthetic-password") {
					t.Fatal("opaque receipt exposed native credentials")
				}
				proof.Close()
				if proof.Prefix() != "/webdav" || proof.Check(r) == nil {
					t.Fatal("closed facade prefix DATA became current applicability")
				}
				// Real captures prove both actual-peer canonical accounting and global64.
				held := make([]*DeploymentWebDAVApplicability, 0, 64)
				t.Cleanup(func() {
					for _, p := range held {
						p.Close()
					}
				})
				for host := 1; host <= 8; host++ {
					for port := 0; port < 8; port++ {
						next := deploymentWebDAVOriginalRequest(t, 30*time.Second, fmt.Sprintf("127.0.0.%d:%d", host, 22000+port))
						p, e := f.issuer.Capture(next)
						if e != nil {
							t.Fatal("bounded original admission", e)
						}
						held = append(held, p)
					}
					if host == 1 {
						mapped := deploymentWebDAVOriginalRequest(t, 30*time.Second, "::ffff:127.0.0.1")
						mapped.Header.Set("X-Forwarded-For", "203.0.113.9")
						if p, e := f.issuer.Capture(mapped); !errors.Is(e, datastore.ErrDeploymentWebDAVBusy) || p != nil {
							if p != nil {
								p.Close()
							}
							t.Fatal("same actual canonical peer bypassed eight while global budget was not full", e)
						}
					}
				}

				extra := deploymentWebDAVOriginalRequest(t, 30*time.Second, "127.0.0.99:25000")
				if p, e := f.issuer.Capture(extra); !errors.Is(e, datastore.ErrDeploymentWebDAVBusy) || p != nil {
					if p != nil {
						p.Close()
					}
					t.Fatal("global original operation cap bypass", e)
				}
				for _, p := range held {
					p.Close()
				}
				held = nil
				if p, e := f.issuer.Capture(extra); e != nil {
					t.Fatal("joined original admission did not refund", e)
				} else {
					p.Close()
				}
			case "different-finalized-request":
				clone := r.Clone(r.Context())
				if err := proof.Check(clone); err == nil {
					t.Fatal("cloned finalized request reused original observation")
				}
				original := deploymentWebDAVOriginalRequest(t, 30*time.Second, "127.0.0.1:21002")
				current := deploymentWebDAVOriginalProof(t, f, original)
				original.Header.Set("Range", "bytes=1-2")
				if err := current.Check(original); err == nil {
					t.Fatal("post-capture request metadata mutation accepted")
				}
			case "fixed-expiry-no-renew":
				expected, _ := r.Context().Deadline()
				if !proof.ExpiresAt().Equal(expected) {
					t.Fatal("original expiry changed")
				}
				if err := proof.Check(r); err != nil {
					t.Fatal(err)
				}
				<-r.Context().Done()
				if proof.Check(r) == nil || proof.Context().Err() == nil || !proof.ExpiresAt().Equal(expected) || proof.Prefix() != "/webdav" {
					t.Fatal("later check renewed original applicability")
				}
				proof.Close()
				if !proof.ExpiresAt().Equal(expected) || proof.Prefix() != "/webdav" {
					t.Fatal("closed receipt recomputed fixed expiry DATA")
				}
			case "close-joins-owned-query":
				pool := f.store.Pool()
				if pool.Config().MaxConns != 1 {
					t.Fatal("actual pool-one canary required")
				}
				held, err := pool.Acquire(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer held.Release()
				before := pool.Stat().CanceledAcquireCount()
				checked := make(chan struct{})
				var checkErr error
				t.Cleanup(func() {
					f.issuer.Close()
					held.Release()
					select {
					case <-checked:
					case <-time.After(10 * time.Second):
						t.Error("original pool-wait operation did not join during failure cleanup")
					}
				})
				go func() { defer close(checked); checkErr = proof.Check(r) }()
				// Observe the actual native shared lock without taking a competing lock.
				// This proves the query operation reached its own genuine progress hold
				// and is waiting on the one held actual PG connection, not a fake callback.
				until := time.Now().Add(3 * time.Second)
				for !deploymentWebDAVNativeReadHeld(t, f.boot.manager.ConfigPath()) && time.Now().Before(until) {
					select {
					case <-checked:
						t.Fatal("original check ended before actual pool wait", checkErr)
					case <-time.After(time.Millisecond):
					}
				}
				if !deploymentWebDAVNativeReadHeld(t, f.boot.manager.ConfigPath()) {
					t.Fatal("actual original progress hold not observed")
				}
				f.issuer.Close()
				select {
				case <-checked:
				case <-time.After(time.Second):
					t.Fatal("closed issuer did not join actual original query operation")
				}
				if checkErr == nil || proof.Context().Err() == nil || pool.Stat().CanceledAcquireCount() <= before || deploymentWebDAVNativeReadHeld(t, f.boot.manager.ConfigPath()) {
					t.Fatal("query/native work survived original Close/refund", checkErr)
				}
				held.Release()
				guard, err := f.startup.source.Coordinator().TryWrite(t.Context())
				if err != nil {
					t.Fatal("closed query retained original settings hold", err)
				}
				_ = guard.Close()
			}
		})
	}
}
